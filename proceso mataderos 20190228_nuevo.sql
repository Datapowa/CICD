--Cogemos todos los mataderos
declare @matadero varchar(14), 
		@fc_mensaje varchar(250)

declare cuMataderos cursor
	for select distinct(codrega), Fecha_msg_explot
	from TV_REGA
	order by codrega,Fecha_msg_explot asc
open cuMataderos

fetch next from cuMataderos
into @matadero,@fc_mensaje

while @@FETCH_STATUS=0
begin

declare @nombre varchar(50),
		@direccion varchar (250),
		@cMunicipio varchar(5),
		@titular varchar(100),
		@nifTitular varchar(9),
		@telefono varchar(9),
		@cp varchar(5),
		@fcAlta datetime,--timestamp
		@establecimiento int,
		@actMatadero int, --El id de la actividad de matadero
		@msgLog varchar(500) --
	
	-- Los datos básicos del establecimiento (en T_ESTAB)
	select @nombre=Coalesce(max(nombre),'Sin datos') from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje
	select @direccion=Coalesce(max(direccion),'Sin datos') from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje
	select @cMunicipio=CAST(provincia as CHAR(2))+ CAST(municipio as CHAR(3)) from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje group by provincia,municipio
	select @nifTitular=max(nif) from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje
	select @titular=max(Titular) from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje
	select @telefono=coalesce(left(tlf,9),'999999999') from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje
	select @cp=Coalesce(max(codigopostal), CAST(provincia as CHAR(2)) +'001') from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje group by provincia
	select @fcAlta= min(convert(datetime,Fecha_estado,120)) from TV_REGA where codrega= @matadero and Fecha_msg_explot=@fc_mensaje

	set @actMatadero=65
	
	--si es uno ya existente y en activo
	IF (select count(es_id) from T_ESTAB where es_CodSan=@matadero and es_Baja is null)>0
	 begin 
		select @establecimiento=es_id from T_ESTAB where es_CodSan=@matadero and es_Baja is null
		--Guardamos lo existente en la tabla de histórico
		insert [TH_REGA]([nombre],[direccion],[Titular],[nif],[tlf],productos,establecimiento,procesado,mensaje)
		select es_Nombre,es_Direccion,es_nombre_titular,es_nif_titular,es_Telefono,dbo.fu_concatenar_codigos_productos(es_id,@actMatadero)
		,es_id,getdate(),@fc_mensaje
		from T_ESTAB
		where es_Id=@establecimiento
		
		--actualizamos los datos
		Update T_ESTAB 
		set es_Nombre=@nombre,es_Direccion=@direccion,es_nombre_titular=@titular,es_nif_titular=@nifTitular,es_Telefono=@telefono
		where es_Id=@establecimiento
		--Le quitamos la actividad de matadero y las categorías y productos de la misma
		delete TR_ESTAB_TIPOACTIVIDAD where EA_TE_ID=@actMatadero and ea_es_id=@establecimiento
		delete TR_CODCAT_ESTAB where CE_COD_TIPOESTAB=@actMatadero and CE_CE_ESTAB=@establecimiento
		delete tr_codpro_estab where PE_CE_COD_TIPOESTAB=@actMatadero and pe_ce_Estab=@establecimiento	
		--print 'Actualizado: '+@matadero
		set @msgLog= 'Actualización'
	 end
	else
	 begin
		if (select count(codrega) from TV_REGA where codrega=@matadero and Fecha_msg_explot=@fc_mensaje and estado='00')>0
		--Si alguno de las filas nos viene como Alta (0) lo damos de alta, si no pasando.
		begin
			--Si existe pero está de baja, le damos de alta
			IF (select count(es_id) from T_ESTAB where es_CodSan=@matadero)>0
			 begin
				select @establecimiento=max(es_id) from T_ESTAB where es_CodSan=@matadero
				Update T_ESTAB 
				set es_Nombre=@nombre,es_Direccion=@direccion,es_nombre_titular=@titular,es_nif_titular=@nifTitular,es_Telefono=@telefono,
				es_auto_id=1,es_Baja=null,es_motivo_baja=null,es_ce_usuario_baja=null
				where es_Id=@establecimiento
				--Le quitamos la actividad de matadero y las categorías y productos de la misma
				delete TR_ESTAB_TIPOACTIVIDAD where EA_TE_ID=@actMatadero and ea_es_id=@establecimiento
				delete TR_CODCAT_ESTAB where CE_COD_TIPOESTAB=@actMatadero and CE_CE_ESTAB=@establecimiento
				delete tr_codpro_estab where PE_CE_COD_TIPOESTAB=@actMatadero and pe_ce_Estab=@establecimiento	
				set @msgLog= 'Reactivado'
			 end
			else
			 begin
				insert T_ESTAB (es_CodSan,es_CodLoc,es_Nombre,es_Direccion,es_ce_Mun,es_nif_titular,es_nombre_titular,es_Telefono,es_CP,es_auto_fecha_alta,es_fec_alta,ES_ORGADMIN)
				values (@matadero,'',@nombre,@direccion,@cMunicipio,@nifTitular,@titular,@telefono,@cp,@fcAlta,getdate(),'Consejería de Sanidad')
				set @establecimiento= @@IDENTITY
				--print 'Creado: '+@matadero
				set @msgLog= 'Creado'
			 end
		end
	 end
	 
If @establecimiento is not null
 begin
	--le ponemos la actividad de matadero
	insert TR_ESTAB_TIPOACTIVIDAD (EA_ES_ID,EA_TE_ID,EA_ESAC_ID)
	values (@establecimiento,@actMatadero,100)
	
	--le ponemos las categorías 
	insert TR_CODCAT_ESTAB (CE_CE_ESTAB,CE_COD_TIPOESTAB,CE_CA_ID)
	values (@establecimiento,@actMatadero,1)
	insert TR_CODCAT_ESTAB (CE_CE_ESTAB,CE_COD_TIPOESTAB,CE_CA_ID)
	values (@establecimiento,@actMatadero,2)
	insert TR_CODCAT_ESTAB (CE_CE_ESTAB,CE_COD_TIPOESTAB,CE_CA_ID)
	values (@establecimiento,@actMatadero,3)
	
	--le ponemos los productos
	--Primero los de todas las especies
		--CAD
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,8)
		--BHHP
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,2)
		--DECOM
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,78)
		--DTC
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,13)
		--EST-GAST
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,79)
		--EST-TRIP
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,80)
		--HISKR
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,24)
		--MANU
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,29)
		--MEZCLA 1
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,85)
		--MEZCLA 2
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,86)
		--MEZCLA 3
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,87)
		--PETR
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,59)
		--REC-CARNE
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,82)	
		--REC-GRASA
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,83)
		--WWT
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,65)
		--C2 biogás/compost 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,92)
		
	--CABE-PATAS
	IF (select count(codrega) from TV_REGA where especie in ('Gallinas','Pavos','Pintadas','Patos','Ocas','Codornices',
	'Palomas','Faisanes','Perdices','Ratites','Conejos','Liebres') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,77)
	end
	-- CERD
	IF (select count(codrega) from TV_REGA where especie in ('Cerdos','Jabalíes') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,71)
	end
	-- PELO
	IF (select count(codrega) from TV_REGA where especie in ('Cerdos','Jabalíes','Équidos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,74)
	end
	-- LANA
	IF (select count(codrega) from TV_REGA where especie in ('Ovinos','Caprinos','Ciervos','Muflones','Corzos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,72)
	end
	-- MER
	IF (select count(codrega) from TV_REGA where especie in ('Ovinos','Caprinos','Ciervos','Muflones','Corzos','Bovinos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,81)
	end
	-- PLUM
	IF (select count(codrega) from TV_REGA where especie in ('Gallinas','Pavos','Pintadas','Patos','Ocas','Codornices',
	'Palomas','Faisanes','Perdices','Ratites') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,75)
	end
	--SANGRE
	IF (select count(codrega) from TV_REGA where especie in ('Gallinas','Pavos','Pintadas','Patos','Ocas','Codornices',
	'Palomas','Faisanes','Perdices','Ratites','Conejos','Liebres','Ovinos','Caprinos','Ciervos','Muflones','Corzos','Bovinos',
	'Cerdos','Jabalíes') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,84)
	end
	-- SERE
	IF (select count(codrega) from TV_REGA where especie in ('Équidos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin 
		insert tr_codpro_estab
		values (@establecimiento,@actMatadero,62)
	end

	--Metemos las especies asociadas a los establecimientos	
	if (select count(codrega) from TV_REGA where especie in ('Bovinos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=1
		insert TR_ESTAB_ESPECIE values (@establecimiento,1)
	 end
	 if (select count(codrega) from TV_REGA where especie in ('Caprinos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=2
		insert TR_ESTAB_ESPECIE values (@establecimiento,2)
	 end
	if (select count(codrega) from TV_REGA where especie in ('Ovinos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=3
		insert TR_ESTAB_ESPECIE values (@establecimiento,3)
	 end
	if (select count(codrega) from TV_REGA where especie in ('Cerdos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=4
		insert TR_ESTAB_ESPECIE values (@establecimiento,4)
	 end
	if (select count(codrega) from TV_REGA where especie in ('Conejos','Liebres') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin --conejos
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=5
		insert TR_ESTAB_ESPECIE values (@establecimiento,5)
	 end
	if (select count(codrega) from TV_REGA where especie in ('Équidos') and estado='00' and codrega=@matadero  and Fecha_msg_explot=@fc_mensaje)>0
	 begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=6
		insert TR_ESTAB_ESPECIE values (@establecimiento,6)
	 end
	IF (select count(codrega) from TV_REGA where especie in ('Gallinas','Pavos','Pintadas','Patos','Ocas','Codornices',
'Palomas','Faisanes','Perdices') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	 begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=7
		insert TR_ESTAB_ESPECIE values (@establecimiento,7)
	 end
	IF (select count(codrega) from TV_REGA where especie in ('Jabalíes','Ciervos','Muflones','Corzos') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=13
		insert TR_ESTAB_ESPECIE values (@establecimiento,13)
	 end
	IF (select count(codrega) from TV_REGA where especie in ('Ratites') and estado='00' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	 begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=15
		insert TR_ESTAB_ESPECIE values (@establecimiento,15)
	 end

  --Quitamos las especies asociadas a los establecimientos, que tengan baja '01'
	if (select count(codrega) from TV_REGA where especie in ('Bovinos') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=1
	 end
	 if (select count(codrega) from TV_REGA where especie in ('Caprinos') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=2
	 end
	if (select count(codrega) from TV_REGA where especie in ('Ovinos') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=3
	 end
	if (select count(codrega) from TV_REGA where especie in ('Cerdos') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin 
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=4
	 end
	if (select count(codrega) from TV_REGA where especie in ('Conejos','Liebres') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin --conejos
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=5
	 end
	if (select count(codrega) from TV_REGA where especie in ('Équidos') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje )>0
	 begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=6
	 end
	IF (select count(codrega) from TV_REGA where especie in ('Gallinas','Pavos','Pintadas','Patos','Ocas','Codornices',
'Palomas','Faisanes','Perdices') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	 begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=7
	 end
	IF (select count(codrega) from TV_REGA where especie in ('Jabalíes','Ciervos','Muflones','Corzos') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=13
	 end
	IF (select count(codrega) from TV_REGA where especie in ('Ratites') and estado='01' and codrega=@matadero and Fecha_msg_explot=@fc_mensaje)>0
	 begin
		delete TR_ESTAB_ESPECIE where ee_ce_Estab=@establecimiento and ee_ce_Especies=15
	 end
--Comprobamos si no tienen ninguna especie como alta en los mensajes
	IF (select count(codrega) from TV_REGA where codrega=@matadero and Fecha_msg_explot=@fc_mensaje and estado='00')=0
	 begin
		--miramos si tiene más actividades
		if (select count(EA_ES_ID) from TR_ESTAB_TIPOACTIVIDAD  where EA_ES_ID=@establecimiento and EA_TE_ID!=@actMatadero)>0
		 begin
		 --Le quitamos la actividad de matadero
			Delete TR_ESTAB_TIPOACTIVIDAD  where EA_ES_ID=@establecimiento and EA_TE_ID=@actMatadero
			delete TR_CODCAT_ESTAB where CE_COD_TIPOESTAB=@actMatadero and CE_CE_ESTAB=@establecimiento
			delete tr_codpro_estab where PE_CE_COD_TIPOESTAB=@actMatadero and pe_ce_Estab=@establecimiento	
			--print 'Baja de actividad: '+@matadero
			set @msgLog='Baja de actividad'
		 end
		else
		 -- Le damos de baja
		 begin
			Update T_ESTAB 
			set es_Baja=GETDATE(),es_motivo_baja='Volcado desde REGA',es_auto_id=2
			where es_Id=@establecimiento
			--print 'Dado de baja: '+@matadero
			set @msgLog='Dado de baja'
		 end
	 end
end

-- "Movemos" las filas usadas a la tabla de log
INSERT [TL_REGA]([codrega],[Fecha_msg_explot],[nombre],[direccion],[especie],[CA],[provincia],[municipio],
[CodigoPostal],[Fecha_estado],[estado],[Titular],[nif],[tlf])
select * from TV_REGA where codrega=@matadero

update TL_rega
set establecimiento=@establecimiento,procesado=GETDATE(),acciones=@msgLog
where codrega=@matadero and Fecha_msg_explot=@fc_mensaje and procesado is null

delete TV_REGA where codrega=@matadero and Fecha_msg_explot=@fc_mensaje

set @establecimiento = null

fetch next from cuMataderos
into @matadero,@fc_mensaje
end
close cuMataderos
deallocate cuMataderos